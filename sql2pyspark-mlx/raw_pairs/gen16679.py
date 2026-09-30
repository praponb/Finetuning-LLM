from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.col("level").alias("label"))
    .unionByName(
        df_shipments
        .filter(~F.col("carrier").isin(["DHL", "UPS", "Kerry"]))
        .select(F.col("carrier").alias("label"))
    )
    .orderBy("label")
)
