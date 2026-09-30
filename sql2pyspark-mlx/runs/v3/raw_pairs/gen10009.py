from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["basic", "premium", "standard"]))
        .select("end_date")
    )
)
