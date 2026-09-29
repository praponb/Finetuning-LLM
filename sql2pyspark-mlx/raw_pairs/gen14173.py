from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("employee_hire_date").desc(), "id")
w3 = Window.partitionBy("level").orderBy(F.col("l1_count").desc())

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.lower(F.col("ch.full_name")).like("%im %")), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"), F.col("c.level"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") >= F.col("s.employee_hire_date")), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("budget").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "employee_hire_date",
        "level",
        F.lower("location").alias("location_lower"),
        F.floor("budget").alias("budget_whole"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name", "level", "location_lower")
)

result = (
    deduped
    .groupBy("level", "dept_name")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .withColumn("l1_count_dense_rank", F.dense_rank().over(w3))
)
