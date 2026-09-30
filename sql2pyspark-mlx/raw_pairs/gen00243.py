from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.when(F.col("plan").rlike("^(standard|premium)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_B").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
