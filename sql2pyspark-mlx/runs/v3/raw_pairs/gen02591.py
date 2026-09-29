from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.length("product_name") >= 11) & F.upper("product_name").like("%UNN%")) | (F.col("cost") < 540))
    .select(
        "id",
        "launch_date",
        "supplier_id",
        "product_name",
        (F.col("cost") + F.col("price")).alias("margin"),
    )
    .orderBy(F.col("id").desc(), "product_name")
)
