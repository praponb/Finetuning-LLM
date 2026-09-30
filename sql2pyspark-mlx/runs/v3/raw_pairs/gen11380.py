from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 20, 21, 37]))
    .select("browser")
    .intersect(
        df_sessions
        .filter(F.col("device").isin(["tablet", "desktop"]))
        .select("browser")
    )
    .orderBy("browser")
)
