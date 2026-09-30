from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost_running = Window.orderBy("unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_is_active_flag_runnin = Window.orderBy(F.col("is_active_flag").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_is_active_flag_prior = Window.orderBy(F.col("is_active_flag").desc(), "id").rowsBetween(-1, -1)
w_category_is_active_flag_by_u = Window.partitionBy("category", "is_active_flag").orderBy("unit_cost", "id")
w_category_is_active_flag = Window.partitionBy("category", "is_active_flag")
w_category_by_unit_cost_prior = Window.partitionBy("category").orderBy("unit_cost", "id").rowsBetween(-1, -1)
w_all_by_is_active_flag = Window.orderBy(F.col("is_active_flag").desc(), "id")
w_is_active_flag_category_by_u = Window.partitionBy("is_active_flag", "category").orderBy("unit_cost", "id").rowsBetween(-2, -1)
w_all_by_unit_cost_ties = Window.orderBy(F.col("unit_cost").asc_nulls_last())
w_category_by_unit_cost = Window.partitionBy("category").orderBy("unit_cost", "id")

valid_skus = (
    df_skus
    .filter(F.size(F.col("labels")) >= 2)
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
    )
)

ranked = (
    valid_skus
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        F.min("unit_cost").over(w_all_by_unit_cost_running).alias("min_unit_cost_so_far"),
        F.first("unit_cost", ignorenulls=True).over(w_all_by_is_active_flag_runnin).alias("first_known_unit_cost"),
        F.sum("is_active_flag").over(w_all_by_is_active_flag_prior).alias("is_active_flag_sum_1_prior"),
        F.row_number().over(w_category_is_active_flag_by_u).alias("unit_cost_seq"),
        F.last("unit_cost", ignorenulls=True).over(w_all_by_unit_cost_running).alias("last_known_unit_cost"),
        F.sum("unit_cost").over(w_category_is_active_flag).alias("category_is_active_flag_sum_unit_cost"),
        F.round(F.avg("is_active_flag").over(w_category_by_unit_cost_prior), 2).alias("is_active_flag_avg_1_prior"),
        F.lag("unit_cost", 2).over(w_all_by_is_active_flag).alias("prev2_unit_cost"),
        F.min("unit_cost").over(w_category_is_active_flag).alias("category_is_active_flag_min_unit_cost"),
        F.max("unit_cost").over(w_is_active_flag_category_by_u).alias("unit_cost_max_2_prior"),
        F.dense_rank().over(w_all_by_unit_cost_ties).alias("unit_cost_dense_rank"),
        F.lead("unit_cost", 2).over(w_category_by_unit_cost).alias("next2_unit_cost"),
    )
)

tagged = (
    ranked
    .withColumn("category_group", F.when(F.col("category").isin("Food", "Toys"), "group_a").when(F.col("category") == "Beauty", "group_b").otherwise("other"))
)

result = (
    tagged
    .select(
        "id",
        "category",
        "unit_cost",
        "labels",
        "min_unit_cost_so_far",
        "first_known_unit_cost",
        "is_active_flag_sum_1_prior",
        "unit_cost_seq",
        "last_known_unit_cost",
        "category_is_active_flag_sum_unit_cost",
        "is_active_flag_avg_1_prior",
        "category_is_active_flag_min_unit_cost",
        "unit_cost_max_2_prior",
        "unit_cost_dense_rank",
        "next2_unit_cost",
        F.greatest(F.col("unit_cost_seq"), F.col("min_unit_cost_so_far")).alias("greatest_unit_cost_seq_min_unit_cost_so_far"),
        F.when(F.col("last_known_unit_cost") >= 277.0, "HIGH").when(F.col("last_known_unit_cost") >= 158.7, "MEDIUM").otherwise("LOW").alias("last_known_unit_cost_tier"),
    )
)
