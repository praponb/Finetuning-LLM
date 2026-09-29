from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["TH", "DE"]))
    .select("browser")
    .subtract(
        df_sessions
        .filter(~F.col("browser").isin(["Chrome", "Edge"]))
        .select("browser")
    )
)
