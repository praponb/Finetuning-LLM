from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date").between("2025-01-01", "2025-08-01"))
        .select("end_date")
    )
)
