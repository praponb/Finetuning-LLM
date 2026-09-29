from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2023-09-01")
    .groupBy(
        F.quarter("delivered_date").alias("delivered_quarter"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") > 230)
    .select("delivered_quarter", "shipped_month", "avg_cost", "total_cost")
    .orderBy(F.col("delivered_quarter").desc(), F.col("total_cost").desc())
)
