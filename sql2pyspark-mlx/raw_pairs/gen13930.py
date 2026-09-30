from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("city") == "Bangkok")
        .select("id", "region")
    )
)
