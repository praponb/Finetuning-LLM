from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("country").isin(["SG", "US"]), "Group A").otherwise("Other").alias("country_group"),
        F.when(F.col("rating") >= 4.5, "High").when(F.col("rating") >= 2.0, "Medium").otherwise("Low").alias("rating_tier"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") > 440), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.month(F.col("c.launch_date")).isin([4, 9, 12])), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"))
)

result = with_latest.select("id", "supplier_name", "country", "rating")
