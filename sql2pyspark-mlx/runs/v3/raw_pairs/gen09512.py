from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(
        df_sessions
        .filter(~F.col("browser").isin(["Safari", "Chrome", "Edge"]))
        .select("country")
    )
    .distinct()
)
