from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.clean_supplier_name",
        "s.supplier_name_upper",
        F.col("c.category").alias("product_category"),
    )
)
