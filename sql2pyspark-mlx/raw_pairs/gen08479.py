from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary",
        "is_remote",
        "level",
        "rank_in_group",
        F.when(F.col("full_name").rlike("^(Mia Rossi|Emma Brown)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("full_name_zone"),
    )
)
