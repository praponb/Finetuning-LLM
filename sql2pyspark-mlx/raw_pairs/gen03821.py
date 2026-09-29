from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("shipped_date")).orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .select("id", "shipped_date", "cost", F.row_number().over(w).alias("rn"))
)
