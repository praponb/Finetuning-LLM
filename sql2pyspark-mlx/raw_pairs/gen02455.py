from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

deduped = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "dept_name",
        "budget",
        "rank_in_group",
        F.when(F.col("dept_name").rlike("^(HR|Sales)$"), "PRIORITY").otherwise("GROUP_B").alias("dept_name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "dept_name")
)
