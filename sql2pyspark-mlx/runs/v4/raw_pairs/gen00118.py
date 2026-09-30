from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Sales", "Finance", "Engineering"])
    .agg(F.max("budget").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "location",
        F.col("Sales_total").alias("sales_total"),
        F.col("Sales_cnt").alias("sales_cnt"),
        F.col("Finance_total").alias("finance_total"),
        F.col("Finance_cnt").alias("finance_cnt"),
        F.col("Engineering_total").alias("engineering_total"),
        F.col("Engineering_cnt").alias("engineering_cnt"),
    )
)
