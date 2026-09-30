from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "product_name",
        F.coalesce("brand", F.lit("unassigned")).alias("brand_filled"),
    )
)
