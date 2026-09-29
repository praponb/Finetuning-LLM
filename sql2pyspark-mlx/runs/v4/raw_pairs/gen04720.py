from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w2 = Window.orderBy(F.col("budget").desc(), "id")
w_hist = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("seq_no_filled").desc_nulls_first())
w_ordered = Window.partitionBy("dept_name")

scored = (
    df_departments
    .select(
        "*",
        F.cume_dist().over(w).alias("budget_cume_dist"),
        F.ntile(5).over(w2).alias("budget_bucket"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

valid = (
    scored
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "seq_no",
        (F.col("budget_cume_dist") + F.col("seq_no")).alias("budget_cume_dist_plus_seq_no"),
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

kept_keys = (
    valid
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("budget") <= 2576000)
        .select("id", "location")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary").between(63000, 87000)), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("location").isin(["New York", "London"]))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("seq_no_whole", F.floor("seq_no"))
    .withColumn("location_clean", F.coalesce("location", F.lit("DIRECT")))
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w4).alias("seq_no_filled_cume_dist"),
        F.avg("seq_no_filled").over(w_ordered).alias("dept_name_avg_seq_no_filled"),
    )
)

summary = (
    enriched
    .withColumn("seq_no_whole_tier", F.when(F.col("seq_no_whole") >= 73, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["seq_no", "seq_no_whole_tier"], ["seq_no"], []],
        "seq_no",
        "seq_no_whole_tier",
    )
    .agg(
        F.percentile_approx("budget_cume_dist_plus_seq_no", 0.95).alias("budget_cume_dist_plus_seq_no_p95"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.round(F.sum("dept_name_avg_seq_no_filled") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("dept_name_avg_seq_no_filled_per_engineering"),
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.grouping("seq_no").alias("grouping_id"),
    )
)

result = (
    summary
    .select(
        "seq_no_whole_tier",
        "dept_name_avg_seq_no_filled_per_engineering",
        "engineering_count",
        "grouping_id",
    )
)
