from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["desktop", "mobile"]))
        .select("country")
    )
    .distinct()
    .orderBy(F.col("country").desc())
)
