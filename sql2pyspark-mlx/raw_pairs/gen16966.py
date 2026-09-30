from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity") == 63)
        .select("id")
    )
)
