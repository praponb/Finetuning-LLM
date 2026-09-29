from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["LOST", "IN_TRANSIT"]))
    .select("id", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") >= "2023-11-01")
        .select("id", "shipped_date")
    )
)
