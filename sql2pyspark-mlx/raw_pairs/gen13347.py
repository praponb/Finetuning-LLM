from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("rating_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy("rating_filled")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("rating") <= 3.1)
    .select("id", "country", F.coalesce("rating", F.lit(0)).alias("rating_filled"))
)

enriched = (
    valid
    .select(
        "id",
        "country",
        "rating_filled",
        F.sum("rating_filled").over(w_running).alias("cumulative_rating_filled"),
        F.rank().over(w_grp).alias("rating_filled_rank"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= F.col("s.product_launch_date")), "left_anti")
)

grouped = (
    eligible
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.sum("rating_filled_rank") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("rating_filled_rank_per_jp"),
    )
)

result = (
    grouped
    .select(
        "country",
        F.when(F.col("country").rlike("^(SG|DE)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "GROUP_A").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_last(), F.col("country").desc())
)
