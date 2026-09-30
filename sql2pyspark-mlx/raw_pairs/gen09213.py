from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")

base = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Singapore"))) & F.col("supplier_name").isNotNull())
    .withColumn("rating_int", F.col("rating").cast("int"))
    .withColumn("is_jp", F.when(F.col("country") == "JP", 1).otherwise(0))
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
