from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("location").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("budget").asc_nulls_first())
w6 = Window.partitionBy("dept_name").orderBy("dept_name_max_budget", "id")
w7 = Window.partitionBy("dept_name").orderBy("budget_rank", "id")

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level").isin(["L2", "L3"])), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "dept_name",
        "budget",
        "latest_rank",
        F.first("location", ignorenulls=True).over(w2).alias("first_known_location"),
        F.max("budget").over(w3).alias("dept_name_max_budget"),
        F.last("dept_name", ignorenulls=True).over(w4).alias("last_known_dept_name"),
        F.rank().over(w5).alias("budget_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("latest_rank") != 1)
    .withColumn("next_dept_name_max_budget", F.lead("dept_name_max_budget", 2, 0).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

remaining_keys = (
    scored
    .select("dept_name", "id")
    .subtract(
        df_departments
        .filter(F.col("budget") >= 1814000)
        .select("dept_name", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title").isin(["Analyst", "Manager"])), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") >= 136000), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("budget") >= 4059000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events_2"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
)
