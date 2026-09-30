from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("product_category").orderBy("product_price")
w3 = Window.partitionBy("product_category").orderBy("rank_in_group", "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("rating").isNotNull())
    .withColumn("country_title", F.initcap(F.trim("country")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        F.col("c.price").alias("product_price"),
        F.col("c.category").alias("product_category"),
    )
)

ranked = (
    with_latest
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.lead("product_price").over(w3).alias("next_product_price"),
        F.lag("product_price").over(w3).alias("prev_product_price"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("product_price") >= 1220, "PREMIUM").otherwise("STANDARD").alias("product_price_tier"),
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("product_category")).alias("num_product_categories"),
    )
    .orderBy(F.col("row_count").desc())
)
