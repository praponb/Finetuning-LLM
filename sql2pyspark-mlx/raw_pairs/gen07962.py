from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "DE", "SG"]))
    .select("*", F.lower("supplier_name").alias("supplier_name_lower"))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "supplier_name")
)

filtered = (
    latest
    .filter(F.col("supplier_name").like("%Glob%"))
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") >= 420), "left_anti")
)

report = (
    kept
    .select("id", "supplier_name", "supplier_name_lower")
    .orderBy("id")
)

result = (
    report
    .select("id")
    .orderBy("id")
)
