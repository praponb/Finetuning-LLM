from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("row_seq").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_part = Window.partitionBy(F.month("hire_date")).orderBy(F.col("salary").asc_nulls_last())
w7 = Window.orderBy(F.col("event_seq").desc())
w8 = Window.orderBy(F.col("cumulative_row_seq").asc_nulls_last())
w_hist = Window.orderBy(F.col("event_seq").desc_nulls_first())
w10 = Window.orderBy(F.col("event_seq_percent_rank").asc_nulls_first())
w11 = Window.orderBy(F.col("event_seq").desc_nulls_last())
w12 = Window.orderBy(F.col("event_seq_dense_rank").asc_nulls_last())

enriched = (
    df_departments
    .select(
        "id",
        "budget",
        F.lag("budget").over(w).alias("prev_budget"),
        F.lead("budget").over(w).alias("next_budget"),
        F.row_number().over(w).alias("row_seq"),
        F.last("dept_name", ignorenulls=True).over(w2).alias("last_known_dept_name"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "budget",
        "row_seq",
        F.sum("row_seq").over(w3).alias("cumulative_row_seq"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.salary"))
)

matched_keys = (
    with_last
    .select("id")
    .intersectAll(
        df_departments
        .filter(F.col("budget") <= 1691000)
        .select("id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w_part).alias("salary_dense_rank"),
        F.percent_rank().over(w7).alias("event_seq_percent_rank"),
        F.cume_dist().over(w8).alias("cumulative_row_seq_cume_dist"),
        F.dense_rank().over(w_hist).alias("event_seq_dense_rank"),
    )
)

with_windows_departments = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w10).alias("event_seq_percent_rank_cume_dist"),
        F.dense_rank().over(w11).alias("event_seq_dense_rank_2"),
        F.dense_rank().over(w12).alias("event_seq_dense_rank_dense_rank"),
    )
)

result = (
    with_windows_departments
    .groupBy(
        F.date_trunc("week", "hire_date").alias("week_start"),
        F.when(F.col("event_seq_dense_rank_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("event_seq_dense_rank_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("event_seq_dense_rank", [0.25, 0.5, 0.75]).alias("event_seq_dense_rank_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cumulative_row_seq", [0.25, 0.5, 0.75]).alias("cumulative_row_seq_quartiles"),
        F.percentile_approx("event_seq_dense_rank", [0.25, 0.5, 0.75]).alias("event_seq_dense_rank_quartiles_2"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy("week_start")
)
