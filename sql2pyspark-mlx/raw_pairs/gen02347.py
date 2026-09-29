from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("product_price").desc_nulls_first(), "id")
w2 = Window.partitionBy("category", "product_id").orderBy(F.col("product_price").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-05-21").cast("date"))
    .withColumn("last_restock_period", F.date_trunc("WEEK", "last_restock"))
)

joined = (
    valid
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price").isNull())
    .select("e.*", F.col("p.product_name"), F.col("p.price"))
)

joined_inventory = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.product_name",
        "s.price",
        F.col("p.price").alias("product_price"),
        F.col("p.category"),
    )
)

ranked = (
    joined_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .cube("category")
    .agg(
        F.percentile_approx("product_price", [0.25, 0.5, 0.75]).alias("product_price_quartiles"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("cnt") < 1000)
)
