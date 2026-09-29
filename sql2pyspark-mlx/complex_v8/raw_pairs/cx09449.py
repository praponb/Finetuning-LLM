from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_list_item_pos_gpa_tier = Window.partitionBy("major_list_item_pos", "gpa_tier")
w_gpa_tier_major_list_item_pos = Window.partitionBy("gpa_tier", "major_list_item_pos").orderBy("n_major_biz", "gpa_tier", "major_list_item_pos")
w_gpa_tier_major_list_item_pos_2 = Window.partitionBy("gpa_tier", "major_list_item_pos").orderBy("n_major_biz", "gpa_tier", "major_list_item_pos").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_students
    .filter((F.col("major").isin("CS", "Math")) & (F.upper(F.trim(F.col("student_name"))).like("L%")))
    .select(
        "id",
        "major",
        "enrolled_on",
        "is_international",
        F.when(F.col("cohort").isin("C2024", "C2021"), "group_a").when(F.col("cohort") == "C2022", "group_b").otherwise("other").alias("cohort_group"),
        F.when(F.col("gpa") >= 3.2, "HIGH").when(F.col("gpa") >= 2.7, "MEDIUM").otherwise("LOW").alias("gpa_tier"),
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
    )
)

summary = (
    scoped
    .groupBy("gpa_tier")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("major") != "Biz", 1)).alias("n_major_biz"),
        F.array_sort(F.collect_set("major")).alias("major_list"),
    )
)

exploded = (
    summary
    .select(
        "gpa_tier",
        "row_count",
        "n_major_biz",
        F.posexplode("major_list").alias("major_list_item_pos", "major_list_item"),
    )
)

ranked = (
    exploded
    .select(
        "gpa_tier",
        "n_major_biz",
        "major_list_item_pos",
        "major_list_item",
        F.sum("row_count").over(w_major_list_item_pos_gpa_tier).alias("major_list_item_pos_gpa_tier_sum_row_count"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_major_list_item_pos_gpa_tier), 2).alias("row_count_vs_major_list_item_pos_gpa_tier_avg"),
        F.size(F.collect_set("major_list_item").over(w_major_list_item_pos_gpa_tier)).alias("distinct_major_list_item_in_major_list_item_pos_"),
        F.ntile(5).over(w_gpa_tier_major_list_item_pos).alias("n_major_biz_quintile"),
        F.first("n_major_biz").over(w_gpa_tier_major_list_item_pos_2).alias("first_n_major_biz"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.abs(F.col("major_list_item_pos") - 2).alias("major_list_item_pos_distance"),
        (F.floor(F.col("n_major_biz_quintile") / 2) * 2).alias("n_major_biz_quintile_bucket"),
        F.abs(F.col("n_major_biz") - 4).alias("n_major_biz_distance"),
    )
)

annotated = (
    derived
    .select(
        "*",
        F.coalesce(F.col("major_list_item_pos"), F.lit(0)).alias("major_list_item_pos_filled"),
    )
)

major_list_item_stats = (
    annotated
    .groupBy("major_list_item")
    .agg(F.percentile_approx("n_major_biz", 0.5).alias("median_n_major_biz"))
)

vs_group = (
    annotated.alias("s")
    .join(major_list_item_stats.alias("g"), F.col("g.major_list_item") == F.col("s.major_list_item"), "inner")
    .filter(F.col("s.n_major_biz") >= F.col("g.median_n_major_biz"))
    .select(
        "s.*",
        "g.median_n_major_biz",
        F.round(F.col("s.n_major_biz") - F.col("g.median_n_major_biz"), 2).alias("n_major_biz_minus_median"),
    )
)

result = (
    vs_group
    .groupBy("n_major_biz_distance", "n_major_biz_quintile")
    .pivot("gpa_tier", ["MEDIUM", "HIGH", "LOW"])
    .agg(F.count("median_n_major_biz"))
    .orderBy("n_major_biz_distance", "n_major_biz_quintile")
)
