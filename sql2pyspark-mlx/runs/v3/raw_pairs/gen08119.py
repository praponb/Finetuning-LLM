from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") < "2025-08-01")
        .select("end_date")
    )
)
