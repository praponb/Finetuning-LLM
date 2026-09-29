from pyspark.sql import functions as F

result = (
    df_stores
    .select("city", "region")
    .intersect(
        df_stores
        .filter(F.col("city").isin(["Singapore", "New York"]))
        .select("city", "region")
    )
)
