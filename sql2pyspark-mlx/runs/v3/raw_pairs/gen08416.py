from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("cost").between(230, 480) & F.col("supplier_id").isNotNull())
)

agg = (
    base
    .groupBy(
        F.when(F.col("price") >= 1310, "high").when(F.col("price") >= 540, "mid").otherwise("low").alias("price_bucket"),
        "brand",
    )
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = (
    agg
    .select("price_bucket", "brand_list")
    .orderBy(F.col("price_bucket").desc())
)
