from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .intersect(
        df_products
        .filter(F.col("brand").isin(["Umbrella", "Globex"]))
        .select("launch_date")
    )
)
