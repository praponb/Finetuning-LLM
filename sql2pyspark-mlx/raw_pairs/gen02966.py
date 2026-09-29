from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").isNotNull())
    .select("launch_date")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").isNotNull())
        .select("last_restock")
    )
)
