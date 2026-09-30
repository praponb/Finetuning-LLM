from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id", "level").orderBy("salary", "id")
w3 = Window.orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "level").orderBy("salary", "id")
w5 = Window.partitionBy("manager_id", "level").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("week", "employee_hire_date"), "employee_level").orderBy("salary", "id")
w7 = Window.partitionBy(F.month("employee_hire_date"), "level").orderBy("rank_in_group", "id")

filtered = (
    df_employees
    .filter(F.upper("full_name").like("%RAN%"))
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.upper("full_name").alias("full_name_upper"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "email", "manager_id", "salary", "hire_date", "level")
)

ranked_employees = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.manager_id").isin([2, 17, 40]))
    .select(
        "a.*",
        F.col("d.level").alias("employee_level"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)

with_windows = (
    enriched
    .withColumn("salary_bucket", F.ntile(5).over(w3))
    .withColumn("salary_bucket_2", F.ntile(3).over(w4))
    .withColumn("running_salary", F.sum("salary").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

summary = (
    with_windows
    .withColumn("running_salary_tier", F.when(F.col("running_salary") >= 638676, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_salary_tier", "employee_level"], ["running_salary_tier"], []],
        "running_salary_tier",
        "employee_level",
    )
    .agg(
        F.count(F.when(F.col("employee_level") == "L2", 1)).alias("l2_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("level") == "L4", F.col("row_seq"))).alias("l4_total_row_seq"),
        F.max(F.when(F.col("employee_level") == "L2", F.col("seq_no"))).alias("l2_max_seq_no"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "running_salary_tier",
        "level_list",
        "l4_total_row_seq",
        "l2_max_seq_no",
        "l5_count",
        "grouping_level",
    )
    .orderBy(F.col("l2_max_seq_no").desc_nulls_last())
)
