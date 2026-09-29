from pyspark.sql import functions as F

result = (
    df_stores
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["Bangkok", "Singapore", "London"]))
        .select("city", "region")
    )
)
