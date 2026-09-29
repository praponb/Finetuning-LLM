from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_credits_filled = Window.partitionBy("credits", "credits_filled")
w_credits_filled_dept_by_credi = Window.partitionBy("credits_filled", "dept").orderBy("credits_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dept_credits_by_credits_prio = Window.partitionBy("dept", "credits").orderBy("credits", "id").rowsBetween(-1, -1)
w_credits_filled_credits_dista = Window.partitionBy("credits_filled", "credits_distance").orderBy("credits_distance", "id")
w_dept_by_credits_distance = Window.partitionBy("dept").orderBy("credits_distance", "id")
w_credits = Window.partitionBy("credits")
w_dept_credits_by_credits_prio_2 = Window.partitionBy("dept", "credits").orderBy("credits", "id").rowsBetween(-3, -1)
w_dept_credits_by_credits_runn = Window.partitionBy("dept", "credits").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_filled_credits_dista_2 = Window.partitionBy("credits_filled", "credits_distance").orderBy("credits")
w_dept_credits_by_credits = Window.partitionBy("dept", "credits").orderBy("credits", "id")
w_credits_filled_dept_by_credi_2 = Window.partitionBy("credits_filled", "dept").orderBy("credits_filled", "id")

scoped = (
    df_courses
    .filter((F.col("dept").isin("Bio", "Math")) & (F.col("level") == "core"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        "credits_filled",
        "credits_distance",
        F.min("credits_filled").over(w_credits_credits_filled).alias("credits_credits_filled_min_credits_filled"),
        F.nth_value("credits_distance", 2).over(w_credits_filled_dept_by_credi).alias("second_credits_distance"),
        F.sum("credits_filled").over(w_dept_credits_by_credits_prio).alias("credits_filled_sum_1_prior"),
        F.row_number().over(w_credits_filled_credits_dista).alias("credits_distance_seq"),
        F.max("credits").over(w_credits_credits_filled).alias("credits_credits_filled_max_credits"),
        F.lag("credits").over(w_dept_by_credits_distance).alias("prev_credits"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_credits), F.lit(0)), 4).alias("credits_share_of_credits"),
        F.round(F.avg("credits_distance").over(w_dept_credits_by_credits_prio_2), 2).alias("credits_distance_avg_3_prior"),
        F.last("credits_filled", ignorenulls=True).over(w_dept_credits_by_credits_runn).alias("last_known_credits_filled"),
        F.dense_rank().over(w_credits_filled_credits_dista_2).alias("credits_dense_rank"),
        F.row_number().over(w_dept_credits_by_credits).alias("seq_no"),
        F.lag("credits_filled", 3).over(w_credits_filled_dept_by_credi_2).alias("prev3_credits_filled"),
        F.count(F.lit(1)).over(w_credits).alias("rows_in_credits"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_credits_credits_filled), F.lit(0)), 4).alias("credits_share_of_credits_credits_filled"),
    )
)

derived = (
    ranked
    .filter(F.col("second_credits_distance") > 1)
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "credits_filled",
        "credits_credits_filled_min_credits_filled",
        "credits_filled_sum_1_prior",
        "credits_distance_seq",
        "credits_share_of_credits",
        "credits_distance_avg_3_prior",
        "credits_dense_rank",
        "seq_no",
        "prev3_credits_filled",
        "rows_in_credits",
        "credits_share_of_credits_credits_filled",
        F.round(F.col("credits_share_of_credits") * 0.25, 2).alias("credits_share_of_credits_adj"),
        F.length(F.trim(F.col("level"))).alias("level_len"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

result = (
    derived
    .filter(F.col("level_len") >= 3)
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "credits_filled",
        "credits_credits_filled_min_credits_filled",
        "credits_filled_sum_1_prior",
        "credits_share_of_credits",
        "credits_distance_avg_3_prior",
        "credits_dense_rank",
        "seq_no",
        "prev3_credits_filled",
        "rows_in_credits",
        "credits_share_of_credits_credits_filled",
        "credits_share_of_credits_adj",
        "level_len",
    )
    .orderBy(F.col("level_len").asc(), F.col("credits").asc_nulls_last(), "id")
    .limit(10)
)
