from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .select("carrier")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") >= "2023-03-01")
        .select("carrier")
    )
    .orderBy("carrier")
)
