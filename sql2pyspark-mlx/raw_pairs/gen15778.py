from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc())
w_running = Window.partitionBy("country").orderBy("price", "id")
w4 = Window.partitionBy("country").orderBy(F.col("price").desc_nulls_last())
w5 = Window.partitionBy("city").orderBy(F.col("price_rank").asc_nulls_last(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.cost").alias("product_cost"))
)

latest = (
    with_last
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.lead("rating", 2).over(w_running).alias("next_rating"),
        F.rank().over(w4).alias("price_rank"),
    )
)

latest_suppliers = (
    enriched
    .withColumn("row_num_2", F.row_number().over(w5))
    .filter(F.col("row_num_2") == 1)
)

cleaned = (
    latest_suppliers
    .filter(F.length("supplier_name") > 7)
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(" - "), "country"))
    .withColumn("rating_per_price", F.round((F.col("rating") / F.col("price")), 2))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

with_last_suppliers = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.price",
        "s.supplier_name_country",
        "s.rating_per_price",
        "s.city_clean",
        F.col("c.cost").alias("product_cost_2"),
    )
)

latest_match = (
    with_last_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.supplier_name_country",
        "s.city_clean",
        "s.product_cost_2",
        F.col("c.cost"),
        F.col("c.brand"),
    )
)

result = (
    latest_match
    .groupBy("brand")
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows_2"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("product_cost_2")).otherwise(0)).alias("umbrella_product_cost_2"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .orderBy("umbrella_rows_2")
)
