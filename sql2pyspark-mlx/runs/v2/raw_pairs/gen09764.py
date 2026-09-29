from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
        .select("country")
    )
)
