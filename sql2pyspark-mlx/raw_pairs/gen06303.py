from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("budget_level", F.when(F.col("budget") >= 4479000, "High").when(F.col("budget") >= 1515000, "Medium").otherwise("Low"))
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") <= 89000), "left_semi")
)

deduped = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_last = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.job_title"))
)

result = (
    with_last
    .groupBy("dept_name")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
