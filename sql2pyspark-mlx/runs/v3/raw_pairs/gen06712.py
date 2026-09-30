from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Sports", "Grocery", "Fashion"]))
    .select("launch_date")
    .intersect(
        df_accounts
        .filter(F.col("status") != "CLOSED")
        .select(F.col("opened_date").alias("launch_date"))
    )
)
