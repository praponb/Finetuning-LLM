from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP"]))
    .select("country")
    .unionByName(
        df_sessions
        .filter(~F.col("device").isin(["mobile", "desktop", "tablet"]))
        .select("country")
    )
    .orderBy(F.col("country").desc())
)
