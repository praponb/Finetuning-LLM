from pyspark.sql import functions as F

result = (
    df_inventory
    .select("product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity") < 100)
        .select("product_id")
    )
)
