from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("level", "dept_id").orderBy("next_is_remote_flag", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_employees
    .filter(F.col("level").eqNullSafe(F.lit("L2")))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

filtered = (
    base
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-02-14")))
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "level",
        "is_remote_label",
        "level_filled",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.lead("is_remote_flag", 1).over(w).alias("next_is_remote_flag"),
        F.avg("is_remote_flag").over(w2).alias("is_remote_flag_avg_2"),
    )
)

metrics = (
    scored
    .withColumn("last4_max_is_remote_flag_avg_2", F.max("is_remote_flag_avg_2").over(w3))
    .withColumn("last4_min_is_remote_flag_avg_2", F.min("is_remote_flag_avg_2").over(w3))
    .withColumn("last4_sum_is_remote_flag_avg_2", F.sum("is_remote_flag_avg_2").over(w3))
    .withColumn("last4_avg_is_remote_flag_avg_2", F.avg("is_remote_flag_avg_2").over(w3))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.dept_id",
        "a.salary",
        "a.hire_date",
        "a.is_remote_label",
        "a.level_filled",
        "a.next_is_remote_flag",
        "a.last4_max_is_remote_flag_avg_2",
        "a.last4_sum_is_remote_flag_avg_2",
        "a.last4_avg_is_remote_flag_avg_2",
        F.col("b.dept_name"),
    )
)

kept_keys = (
    with_dims
    .select("dept_id", "id")
    .intersect(
        df_employees
        .filter(~F.col("is_remote"))
        .select("dept_id", "id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("YEAR", "hire_date").alias("year_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.round(F.avg("last4_max_is_remote_flag_avg_2"), 2).alias("avg_last4_max_is_remote_flag_avg_2"),
    )
    .filter(F.col("avg_last4_max_is_remote_flag_avg_2") < 1000000)
)
