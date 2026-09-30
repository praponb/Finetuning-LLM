from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("carrier").isin(["UPS", "Flash", "Kerry"]) & F.col("status").isin(["LOST", "IN_TRANSIT", "RETURNED"])) | F.col("cost").between(26, 47)))
    .select(
        "id",
        "shipped_date",
        F.when(F.col("cost") >= 84, "High").when(F.col("cost") >= 28, "Medium").otherwise("Low").alias("cost_tier"),
    )
)
