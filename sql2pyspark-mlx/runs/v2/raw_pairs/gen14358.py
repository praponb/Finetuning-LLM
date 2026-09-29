from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Flash", "Kerry"]))
    .select("id", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "Kerry")
        .select("id", "shipped_date")
    )
    .distinct()
    .orderBy("id")
)
