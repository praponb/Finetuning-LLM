from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location")
w3 = Window.orderBy("row_seq", "id")
w4 = Window.orderBy("row_seq", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_name").orderBy("row_seq", "id").rowsBetween(-2, -1)
w7 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location", "dept_name").orderBy("row_seq_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level") == "L1"), "left_semi")
)

metrics = (
    not_flagged
    .filter(F.col("dept_name") != "Engineering")
    .withColumn("prev_budget", F.lag("budget").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

scored = (
    metrics
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("row_seq_2", F.row_number().over(w3))
    .withColumn("row_seq_avg_6_prior", F.avg("row_seq").over(w4))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w5))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

enriched = (
    scored
    .select(
        "id",
        "dept_name",
        "budget",
        "row_seq",
        "budget_share",
        "row_seq_2",
        "row_seq_avg_6_prior",
        "cumulative_row_seq",
        F.avg("prev_budget").over(w6).alias("prev_budget_avg_2_prior"),
        F.last("cumulative_row_seq", ignorenulls=True).over(w7).alias("last_known_cumulative_row_seq"),
        F.last("budget_share_2", ignorenulls=True).over(w8).alias("last_known_budget_share_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("budget_share") >= 0.68)
    .withColumn("greatest_row_seq_avg_6_prior_budget", F.greatest("row_seq_avg_6_prior", "budget"))
)

result = (
    filtered
    .select(
        "id",
        "dept_name",
        "row_seq",
        "row_seq_2",
        "row_seq_avg_6_prior",
        "cumulative_row_seq",
        "last_known_budget_share_2",
        "greatest_row_seq_avg_6_prior_budget",
        F.when(F.col("dept_name").rlike("^(Sales|Finance)$"), "EXTENDED").when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "GROUP_B").otherwise("GROUP_A").alias("dept_name_zone"),
    )
    .orderBy(
        F.col("greatest_row_seq_avg_6_prior_budget").asc_nulls_first(),
        F.col("cumulative_row_seq").desc(),
    )
)
