from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id")

deduped = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "name",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "rank_in_group",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "CORE").when(F.col("segment").rlike("^(Consumer)$"), "GROUP_B").otherwise("EXTENDED").alias("segment_zone"),
    )
)
