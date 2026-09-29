from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date").between("2024-04-01", "2024-10-01"))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("category").desc())
)
