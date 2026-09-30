from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["West", "Central"]))
    .select("opened_date", "region")
    .unionByName(
        df_stores
        .filter(F.col("region").isNotNull())
        .select("opened_date", "region")
    )
    .distinct()
    .orderBy("region")
)
