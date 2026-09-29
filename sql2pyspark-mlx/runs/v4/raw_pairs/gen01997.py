from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-10-09").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.dayofmonth("hire_date").alias("hire_date_day"),
    )
)

result = (
    filtered
    .groupBy("manager_id")
    .pivot("level", ["L1", "L5"])
    .agg(F.max("salary").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "manager_id",
        F.col("L1_amount").alias("l1_amount"),
        F.col("L1_item_count").alias("l1_item_count"),
        F.col("L5_amount").alias("l5_amount"),
        F.col("L5_item_count").alias("l5_item_count"),
    )
)
