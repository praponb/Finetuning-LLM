from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
    )
    .orderBy(F.col("id").desc())
)
