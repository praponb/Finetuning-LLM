from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((~F.col("carrier").isin(["Flash", "FedEx"]) & (F.col("cost") > 35)) | (F.col("delivered_date") <= "2023-06-02"))
    .select(
        "id",
        "cost",
        "delivered_date",
        "status",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
    .orderBy(F.col("cost").desc(), "delivered_date")
)
