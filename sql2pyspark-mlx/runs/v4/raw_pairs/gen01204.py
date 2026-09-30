from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location", "dept_name")
w5 = Window.partitionBy("location").orderBy(F.col("budget_avg_5_prior_whole").desc_nulls_last(), "id")
w6 = Window.partitionBy("location")
w7 = Window.partitionBy("dept_name").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("budget_bucket", "id")
w9 = Window.partitionBy("location").orderBy("budget_avg_5_prior", "id")
w10 = Window.orderBy(F.col("budget_bucket").desc_nulls_first())

valid = (
    df_departments
    .filter(F.col("location") != "Singapore")
    .withColumn("location_dept_name", F.concat("location", F.lit(", "), "dept_name"))
    .withColumn("location_filled", F.coalesce("location", F.lit("NONE")))
)

scored = (
    valid
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_5_prior"),
        F.ntile(4).over(w2).alias("budget_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.max("budget").over(w4).alias("location_max_budget"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Designer"), "left_anti")
)

valid_departments = (
    not_flagged
    .filter(F.col("location").isNotNull() & (F.col("row_seq") > 5))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_dept_name",
        "location_filled",
        "budget_avg_5_prior",
        "budget_bucket",
        "row_seq",
        F.ceil("budget_avg_5_prior").alias("budget_avg_5_prior_whole"),
        F.coalesce("location", F.lit("NONE")).alias("location_filled_2"),
    )
)

latest = (
    valid_departments
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

scored_departments = (
    latest
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w6)), 4))
    .withColumn("cumulative_budget_avg_5_prior", F.sum("budget_avg_5_prior").over(w7))
    .withColumn("row_seq_2", F.row_number().over(w8))
    .withColumn("budget_avg_5_prior_bucket", F.ntile(5).over(w9))
    .withColumn("budget_bucket_cume_dist", F.cume_dist().over(w10))
)

matched_keys = (
    scored_departments
    .select("location", "id")
    .intersectAll(
        df_departments
        .filter(F.col("budget") > 2340000)
        .select("location", "id")
    )
)

kept_rows = (
    scored_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "dept_name",
        "location",
        "budget",
        "location_dept_name",
        "location_filled",
        "budget_avg_5_prior",
        "row_seq",
        "budget_avg_5_prior_whole",
        "rn",
        F.when(F.col("dept_name").rlike("^(Legal|Marketing)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name_zone").desc_nulls_last())
)
