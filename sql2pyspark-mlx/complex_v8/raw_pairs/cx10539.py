from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_credits_ties = Window.orderBy("max_credits")
w_max_credits = Window.partitionBy("max_credits")
w_all_by_max_credits = Window.orderBy("max_credits", "dept_group")
w_all_by_max_credits_running = Window.orderBy("max_credits", "dept_group").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_courses
    .filter(F.col("dept") == "Bio")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.upper(F.trim(F.col("level"))).alias("level_clean"),
        F.when(F.col("dept").isin("Art", "CS"), "group_a").when(F.col("dept") == "Bio", "group_b").otherwise("other").alias("dept_group"),
    )
)

scored = (
    base
    .filter(F.upper(F.trim(F.col("level_clean"))).like("I%"))
    .withColumn("dept_group_level_clean_label", F.concat_ws("|", F.col("dept_group"), F.col("level_clean")))
)

totals = (
    scored
    .groupBy("dept_group")
    .agg(
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.max("credits").alias("max_credits"),
    )
    .filter(F.col("max_credits") > 2)
)

with_trends = (
    totals
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_max_credits_ties), 3).alias("max_credits_percent_rank"),
        F.size(F.collect_set("dept_group").over(w_max_credits)).alias("distinct_dept_group_in_max_credits"),
        F.row_number().over(w_all_by_max_credits).alias("seq_no"),
        F.last("max_credits", ignorenulls=True).over(w_all_by_max_credits_running).alias("last_known_max_credits"),
    )
)

result = (
    with_trends
    .select(
        "dept_group",
        F.col("distinct_dept_group_in_max_credits").alias("m_distinct_dept_group_in_max_credits"),
        F.col("max_credits").alias("max_credits_v"),
        F.col("last_known_max_credits").alias("m_last_known_max_credits"),
    )
    .unpivot(
        ids=["dept_group"],
        values=["m_distinct_dept_group_in_max_credits", "max_credits_v", "m_last_known_max_credits"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
