from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("stock_qty_tier").orderBy(F.col("cnt").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("Li%"))
    .select(
        "*",
        F.initcap(F.trim("country")).alias("country_title"),
        F.round((F.col("rating") * 0.85), 2).alias("rating_scaled"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") < 580), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"), F.col("c.stock_qty"))
)

summary = (
    latest_match
    .groupBy(
        F.when(F.col("stock_qty") >= 440, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("rating_scaled"), 2).alias("avg_rating_scaled"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
    .withColumn("cnt_rank", F.rank().over(w2))
)

result = (
    summary
    .select("stock_qty_tier", "rating_tier", "jp_events", "cnt_rank")
    .orderBy(F.col("jp_events").asc_nulls_last())
)
