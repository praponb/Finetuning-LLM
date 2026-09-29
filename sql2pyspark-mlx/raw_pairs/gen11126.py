from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("Unknown")))
)

cleaned = (
    base
    .filter(F.col("city").isNotNull() & (F.col("country") == "TH"))
    .withColumn("country_length", F.length("country"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        "s.supplier_name_filled",
        F.col("c.category").alias("product_category"),
        F.col("c.brand").alias("product_brand"),
    )
)

base_suppliers = (
    with_last
    .filter(F.col("product_category").isNotNull())
    .select(
        "id",
        "city",
        "rating",
        "supplier_name_filled",
        "product_category",
        "product_brand",
        F.col("rating").cast("int").alias("rating_int"),
        F.trim("product_brand").alias("clean_product_brand"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

wide = (
    base_suppliers
    .groupBy("product_brand")
    .pivot("city", ["Singapore", "Tokyo"])
    .agg(F.avg("rating_int").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    wide
    .select("product_brand", "Singapore_amount", "Singapore_cnt", "Tokyo_amount")
    .orderBy(F.col("Singapore_amount").desc(), F.col("Tokyo_amount").desc())
)
