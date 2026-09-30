from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("budget") >= 3954000) & F.col("budget").between(2749000, 3617000)))
    .orderBy(F.col("location").desc(), "id")
    .limit(50)
)
