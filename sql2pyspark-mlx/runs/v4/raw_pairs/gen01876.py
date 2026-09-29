from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("manager_id").orderBy(F.col("salary_avg_3_prior").asc_nulls_first(), "id")

enriched = (
    df_employees
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w3))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

base = (
    ranked
    .filter(F.col("salary_avg_3_prior") > 108791)
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "salary_share",
        "seq_no",
        "salary_avg_3_prior",
        F.upper("level").alias("level_upper"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

result = (
    base
    .select("id", "job_title", "salary_share", "salary")
    .unpivot(["id", "job_title"], ["salary_share", "salary"], "measure", "reading")
    .filter(F.col("reading").isNotNull())
)
