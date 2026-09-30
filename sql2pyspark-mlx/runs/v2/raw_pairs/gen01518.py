from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        F.coalesce("product_name", F.lit("unassigned")).alias("product_name_filled"),
    )
)
