from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "stock_qty",
        "category",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
