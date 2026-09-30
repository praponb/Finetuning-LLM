from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

latest = (
    df_suppliers
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "supplier_name",
        "country",
        F.when(F.col("supplier_name").rlike("^(Tokyo Traders|Global Supply)$"), "PRIORITY").otherwise("EXTENDED").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").desc_nulls_last())
)
