from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("manager_id").orderBy("is_remote_flag", "id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("manager_id").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("level_min_is_remote_flag", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-08").cast("date")) & (F.length("email") >= 11))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

base = (
    valid
    .filter(F.col("manager_id").isNotNull() & F.col("full_name").like("FR%"))
    .select(
        "id",
        "full_name",
        "manager_id",
        "is_remote",
        "level",
        "is_remote_flag",
        F.substring("job_title", 1, 4).alias("job_title_prefix"),
        F.coalesce("level", F.lit("unassigned")).alias("level_filled"),
    )
)

enriched = (
    base
    .withColumn("next_is_remote_flag", F.lead("is_remote_flag", 2).over(w_grp))
    .withColumn("level_min_is_remote_flag", F.min("is_remote_flag").over(w2))
    .withColumn("manager_id_stddev_samp_is_remote_flag", F.stddev_samp("is_remote_flag").over(w3))
    .withColumn("running_is_remote_flag", F.sum("is_remote_flag").over(w4))
)

scored = (
    enriched
    .filter(F.col("full_name").isNotNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "is_remote",
        "job_title_prefix",
        "level_filled",
        "next_is_remote_flag",
        "level_min_is_remote_flag",
        F.lag("next_is_remote_flag").over(w5).alias("prev_next_is_remote_flag"),
        F.lead("is_remote_flag").over(w5).alias("next_is_remote_flag_2"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(
        "manager_id",
        F.when(F.col("level_min_is_remote_flag") >= 78, "HIGH").otherwise("LOW").alias("level_min_is_remote_flag_tier"),
    )
    .agg(
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.countDistinct("manager_id").alias("distinct_manager_ids_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("next_is_remote_flag").alias("max_next_is_remote_flag"),
    )
    .orderBy(F.col("distinct_manager_ids").asc_nulls_first())
)
