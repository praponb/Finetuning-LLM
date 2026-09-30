from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(~F.col("country").isin(["SG", "UK", "DE"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "country", "rating", "supplier_name")
    .orderBy(F.col("id").desc(), "country")
)
