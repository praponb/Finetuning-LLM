from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_credits_by_credits_tie = Window.partitionBy("level", "credits").orderBy("credits")
w_level_by_credits_running = Window.partitionBy("level").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_level_credits_by_credits = Window.partitionBy("level", "credits").orderBy("credits", "id")

filtered_courses = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 5) & (F.col("credits") <= 4))
    .select(
        "id",
        "dept",
        "credits",
        "level",
    )
)

sequenced = (
    filtered_courses
    .select(
        "id",
        "dept",
        "level",
        F.round(F.percent_rank().over(w_level_credits_by_credits_tie), 3).alias("credits_percent_rank"),
        F.first("credits", ignorenulls=True).over(w_level_by_credits_running).alias("first_known_credits"),
        F.dense_rank().over(w_level_credits_by_credits_tie).alias("credits_dense_rank"),
        F.lag("credits").over(w_level_credits_by_credits).alias("prev_credits"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.min("prev_credits").alias("overall_min_prev_credits")))
    .filter(F.col("prev_credits") > F.col("overall_min_prev_credits"))
    .drop("overall_min_prev_credits")
)

leaders = qualified.filter(F.col("credits_dense_rank") == 1)

qualified_2 = (
    leaders
    .crossJoin(leaders.agg(F.min("first_known_credits").alias("overall_min_first_known_credits")))
    .filter(F.col("first_known_credits") > F.col("overall_min_first_known_credits"))
    .drop("overall_min_first_known_credits")
)

result = (
    qualified_2
    .filter(F.col("credits_percent_rank") > 0.3)
    .select(
        "id",
        "dept",
        "first_known_credits",
        "credits_dense_rank",
        F.least(F.col("credits_percent_rank"), F.col("credits_dense_rank")).alias("least_credits_percent_rank_credits_dense_rank"),
        F.least(F.col("first_known_credits"), F.col("credits_percent_rank")).alias("least_first_known_credits_credits_percent_rank"),
    )
    .orderBy("dept", F.col("credits_dense_rank").desc(), "id")
    .limit(20)
)
